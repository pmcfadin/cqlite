//! Value-level oracle for the AD2 JSON/CSV egress parity lane (issue #1491,
//! epic #1469).
//!
//! # The gap this closes
//!
//! Before #1491, nothing compared the CLI's JSON/CSV *values* to the
//! `sstabledump` goldens. `one_shot_e2e_tests.rs::validate_json_structure`
//! asserted only "non-empty array of objects, `len <= reference.len()`", and
//! `export_integration_tests.rs::test_export_json_deterministic` /
//! `test_export_csv_deterministic` asserted shape and row counts. A regression in
//! `ValueFormatter` / `value_to_json` (blob hex, decimal text, timestamp
//! rendering, `null` for an absent cell) therefore passed silently.
//!
//! # The oracle, and why it is the right one
//!
//! The committed `*-Data.db.jsonl` files are Apache Cassandra `sstabledump`
//! output — the *physical-dump* oracle. That is deliberately the correct oracle
//! for an **egress formatting** property: the question here is "is this value
//! rendered the way Cassandra renders it", not "is the post-reconciliation result
//! set right" (that is the query-semantics oracle's job — CLAUDE.md, "Two parity
//! oracles"). Every expectation in this module is therefore derived from the
//! golden bytes or from `sstabledump` semantics; **nothing** is derived from
//! CQLite's own current output.
//!
//! Because the physical dump enumerates on-disk cells rather than a reconciled
//! result set, a table whose golden carries a partition/row deletion, a range
//! tombstone, a static block or a TTL is **not** comparable this way — the CLI
//! legitimately returns a different row set. Those tables are excluded BY NAME,
//! with a reason, in the case table of the test target; this module refuses to
//! parse such a golden ([`golden_rows`] returns `Err`) so an exclusion can never
//! be applied silently or accidentally widened.
//!
//! # Normalization: only where two spellings denote the same value
//!
//! * **Timestamps.** `sstabledump` writes a timestamp cell as
//!   `YYYY-MM-DD HH:MM:SS.mmmZ`; the CLI writes `YYYY-MM-DD HH:MM:SS.mmm+0000`
//!   (and CSV the same). Both are the same instant, so both are canonicalized to
//!   `YYYY-MM-DDTHH:MM:SS.mmmZ`. Only a ZERO UTC offset is accepted — a non-zero
//!   offset is left as opaque text so it FAILS loudly rather than being
//!   silently shifted.
//! * **Numeric text vs JSON number.** `sstabledump` renders a collection's cell
//!   *path* (a set element, a map key) as a JSON **string** — `"path": ["-5"]` —
//!   while the CLI renders that same element as a JSON **number**. Numeric texts
//!   are therefore compared NUMERICALLY, via the pure-string
//!   [`normalize_decimal`] (no `10^scale` materialization, no `f64` round-trip, so
//!   a 30-digit `decimal` is exact). Consequence, accepted and recorded: a `text`
//!   column whose value happens to be `"1.0"` compares equal to the number `1`.
//!   That distinction is unreachable for a formatter regression, because a text
//!   cell's bytes pass through unchanged on both sides.
//! * **Map spelling.** `sstabledump` renders a map as a JSON object
//!   (`{"x": 10}`); the CLI renders it as an array of `{"key": …, "value": …}`
//!   pairs. Both are compared as key-sorted pair lists.
//! * **UDT `_type`.** `sstabledump` renders a UDT as a plain field→value object;
//!   the CLI adds a `_type` discriminator naming the type. It is dropped from the
//!   CLI side (and only from the CLI side).
//!
//! Everything else is compared byte-exactly, including blob `0x…` hex, decimal
//! text, booleans, UUID text and `null`.
//!
//! # What is NOT normalized, on purpose
//!
//! The CSV lane compares **scalar** columns only. A collection/UDT in CSV is
//! rendered in a CQLite-specific text syntax (`{a, b}`, `[1, 2]`, `{k: v}`) which
//! has no counterpart in the physical dump's flattened per-path cell form and no
//! external authority whatsoever — the only place that syntax is defined is
//! CQLite's own writer, so "expecting" it would be CQLite-vs-CQLite circularity
//! (CLAUDE.md, format authority). That is a TYPE-level rule, declared once here
//! and reported in the run census, not a per-case escape hatch.

#![allow(dead_code)]

// The #3220 TABLE-granular datasets-root rule, reused rather than re-derived: a
// root is chosen by EVIDENCE (does this table's `*-Data.db` exist under it), never
// by a fixed env-first/checkout-first preference. The nested `#[path]` inside that
// file resolves against its own directory, so the cross-crate include is sound.
#[path = "../../../cqlite-core/tests/support/datasets_root.rs"]
pub mod datasets_root;

/// The comparator + CLI-egress readers + fixture staging (split out to keep both
/// files well inside the campsite-rule size target).
#[path = "golden_value_compare.rs"]
pub mod compare;

use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// A row projected onto `column name → value`, with collections already
/// reconstructed into a container value. Both the golden side and the CLI side
/// are reduced to this shape before comparison.
pub type Row = BTreeMap<String, Value>;

/// The storage shape of a NON-frozen collection column, taken from the committed
/// `CREATE TABLE` in `test-data/schemas/*.cql`.
///
/// Required because `sstabledump` flattens a multi-cell collection into one cell
/// per element and the three kinds are only distinguishable by *where* the element
/// lives: a `set` puts it in the cell `path`, a `list` in the cell `value` (its
/// path being an internal timeuuid), a `map` puts the key in the path and the
/// value in the value. Inferring the kind from "is the value empty" would be
/// exactly the byte-pattern guessing the no-heuristics mandate (#28) forbids, so
/// the kind is DECLARED and an undeclared multi-cell column is a hard error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Multicell {
    Set,
    List,
    Map,
}

/// Which egress format is being compared. Affects scalar canonicalization only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Egress {
    /// `export --format json`: values keep their JSON kind (number/bool/null).
    Json,
    /// `export --format csv`: every cell is text, and an empty cell is `null`.
    Csv,
}

/// A canonical scalar: the unit of value equality.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Canon {
    Null,
    Bool(bool),
    /// A number, canonicalized as exact decimal text (see [`normalize_decimal`]).
    Num(String),
    /// Opaque text: blob hex, UUID, `text`, and a canonicalized timestamp.
    Text(String),
}

impl Canon {
    /// The CSV projection: CSV carries no JSON kinds, so a boolean is compared as
    /// its text spelling and numbers stay numeric (`1` == `"1"`).
    ///
    /// An EMPTY string collapses onto `null`, because CSV cannot distinguish them:
    /// the CLI writes an absent value as an empty field and an empty `text` value
    /// as the same empty field. Cassandra's own CSV egress (`cqlsh COPY TO`) has
    /// exactly this ambiguity, so it is a property of the format, not of CQLite —
    /// and the JSON lane keeps the distinction strict (`null` vs `""`), so it is
    /// still asserted somewhere.
    fn for_csv(self) -> Canon {
        match self {
            Canon::Bool(b) => Canon::Text(b.to_string()),
            Canon::Text(t) if t.is_empty() => Canon::Null,
            other => other,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Canon::Null => "null".to_string(),
            Canon::Bool(b) => format!("bool:{b}"),
            Canon::Num(n) => format!("num:{n}"),
            Canon::Text(t) => format!("text:{t}"),
        }
    }
}

// ===========================================================================
// Scalar canonicalization
// ===========================================================================

/// Canonicalize a JSON scalar. Containers are rejected — the caller decides what a
/// container in a scalar position means.
pub fn canon_scalar(v: &Value, egress: Egress) -> Result<Canon, String> {
    let canon = match v {
        Value::Null => Canon::Null,
        Value::Bool(b) => Canon::Bool(*b),
        Value::Number(n) => match normalize_decimal(&n.to_string()) {
            Some(text) => Canon::Num(text),
            // Unreachable for any JSON number serde can produce; reported rather
            // than silently coerced so an unexpected spelling cannot pass.
            None => return Err(format!("uncanonicalizable JSON number {n}")),
        },
        Value::String(s) => canon_text(s),
        Value::Array(_) | Value::Object(_) => {
            return Err("container value in a scalar position".to_string())
        }
    };
    Ok(match egress {
        Egress::Json => canon,
        Egress::Csv => canon.for_csv(),
    })
}

/// Canonicalize a textual scalar: a timestamp spelling first, then a numeric
/// spelling, else opaque text.
pub fn canon_text(s: &str) -> Canon {
    if let Some(ts) = canon_timestamp(s) {
        return Canon::Text(ts);
    }
    match normalize_decimal(s) {
        Some(text) => Canon::Num(text),
        None => Canon::Text(s.to_string()),
    }
}

/// Canonicalize `YYYY-MM-DD[ T]HH:MM:SS[.frac](Z|+0000|+00:00)` to
/// `YYYY-MM-DDTHH:MM:SS.fffZ`, or `None` when `s` is not that shape.
///
/// A NON-ZERO offset deliberately returns `None`: shifting it here would silently
/// reinterpret the value, while leaving it opaque makes the comparison fail and
/// name the two spellings.
pub fn canon_timestamp(s: &str) -> Option<String> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    if !(digits(0..4) && b[4] == b'-' && digits(5..7) && b[7] == b'-' && digits(8..10)) {
        return None;
    }
    if !(b[10] == b' ' || b[10] == b'T') {
        return None;
    }
    if !(digits(11..13) && b[13] == b':' && digits(14..16) && b[16] == b':' && digits(17..19)) {
        return None;
    }
    let mut rest = &s[19..];
    let mut frac = String::new();
    if let Some(stripped) = rest.strip_prefix('.') {
        let n = stripped.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 {
            return None;
        }
        frac = stripped[..n].to_string();
        rest = &stripped[n..];
    }
    // Zero UTC offsets only (see the doc comment).
    if !matches!(rest, "Z" | "+0000" | "+00:00" | "-0000" | "-00:00") {
        return None;
    }
    // Trailing zeros in the fraction are not significant; `.000` and no fraction
    // denote the same instant.
    let frac = frac.trim_end_matches('0');
    let date_time = &s[..19];
    let date_time = date_time.replacen(' ', "T", 1);
    if frac.is_empty() {
        Some(format!("{date_time}Z"))
    } else {
        Some(format!("{date_time}.{frac}Z"))
    }
}

/// Largest number of zeros this will pad when re-scaling an exponent. Bounds the
/// allocation so a hostile-looking `1e999999999` in a golden cannot blow up the
/// test process; such an input is reported as non-numeric (opaque text) instead.
const MAX_DECIMAL_PAD: i64 = 4096;

/// Exact decimal canonicalization of a numeric TEXT, or `None` when `s` is not a
/// plain decimal literal (`0x…` hex, `Infinity`, `NaN`, a UUID, … all return
/// `None` and are then compared as opaque text).
///
/// Pure string arithmetic: no `10^scale` materialization and no `f64` round-trip,
/// so a 30-digit `decimal` from a golden keeps every digit. Negative zero is
/// preserved (`-0.0` → `-0`), because Cassandra distinguishes `-0.0` from `0.0`
/// and the goldens contain both.
pub fn normalize_decimal(s: &str) -> Option<String> {
    let mut rest = s;
    let negative = match rest.as_bytes().first() {
        Some(b'-') => {
            rest = &rest[1..];
            true
        }
        Some(b'+') => {
            rest = &rest[1..];
            false
        }
        _ => false,
    };
    let (mantissa, exp_text) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], Some(&rest[i + 1..])),
        None => (rest, None),
    };
    let (int_part, frac_part) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|c| c.is_ascii_digit())
        || !frac_part.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let exp: i64 = match exp_text {
        None => 0,
        Some(t) => {
            if t.is_empty() {
                return None;
            }
            let (sign, mag) = match t.as_bytes()[0] {
                b'-' => (-1i64, &t[1..]),
                b'+' => (1i64, &t[1..]),
                _ => (1i64, t),
            };
            if mag.is_empty() || !mag.bytes().all(|c| c.is_ascii_digit()) {
                return None;
            }
            // A magnitude too large to matter is refused, not saturated: a
            // saturated exponent would silently change the value.
            let mag: i64 = mag.parse().ok()?;
            sign.checked_mul(mag)?
        }
    };

    let digits: String = format!("{int_part}{frac_part}");
    let point = i64::try_from(int_part.len()).ok()?.checked_add(exp)?;
    let len = i64::try_from(digits.len()).ok()?;

    let text = if point <= 0 {
        let pad = point.checked_neg()?;
        if pad > MAX_DECIMAL_PAD {
            return None;
        }
        format!("0.{}{}", "0".repeat(pad as usize), digits)
    } else if point >= len {
        let pad = point.checked_sub(len)?;
        if pad > MAX_DECIMAL_PAD {
            return None;
        }
        format!("{}{}", digits, "0".repeat(pad as usize))
    } else {
        let cut = point as usize;
        format!("{}.{}", &digits[..cut], &digits[cut..])
    };

    // Trim to a single canonical spelling per value.
    let text = if text.contains('.') {
        let trimmed = text.trim_end_matches('0');
        trimmed.trim_end_matches('.').to_string()
    } else {
        text
    };
    let (whole, fraction) = match text.split_once('.') {
        Some((w, f)) => (w, Some(f)),
        None => (text.as_str(), None),
    };
    let whole_trimmed = whole.trim_start_matches('0');
    let whole_out = if whole_trimmed.is_empty() {
        "0"
    } else {
        whole_trimmed
    };
    let body = match fraction {
        Some(f) => format!("{whole_out}.{f}"),
        None => whole_out.to_string(),
    };
    Some(if negative { format!("-{body}") } else { body })
}

// ===========================================================================
// Golden (sstabledump JSONL) → rows
// ===========================================================================

/// Parse a `*-Data.db.jsonl` golden into comparable rows.
///
/// `Err` means the golden is NOT comparable to a reconciled CLI result set (a
/// partition/row deletion, a range tombstone, a static block, a TTL, an
/// undeclared multi-cell column, a key arity that contradicts the declared
/// schema). It is a hard error rather than a skip so that a table's presence in
/// the parity set is always a decision someone made explicitly.
pub fn golden_rows(
    jsonl: &str,
    pk: &[&str],
    ck: &[&str],
    multicell: &[(&str, Multicell)],
) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();
    for (lineno, line) in jsonl.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let at = || format!("golden line {}", lineno + 1);
        let doc: Value =
            serde_json::from_str(line).map_err(|e| format!("{}: invalid JSON: {e}", at()))?;
        let partition = doc
            .get("partition")
            .ok_or_else(|| format!("{}: no `partition`", at()))?;
        if partition.get("deletion_info").is_some() {
            return Err(format!(
                "{}: partition deletion marker — the physical dump keeps a \
                 partition the CLI's reconciled result set drops",
                at()
            ));
        }
        let keys = partition
            .get("key")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("{}: no `partition.key` array", at()))?;
        if keys.len() != pk.len() {
            return Err(format!(
                "{}: golden partition key arity {} but {} partition column(s) declared ({pk:?})",
                at(),
                keys.len(),
                pk.len()
            ));
        }
        let empty = Vec::new();
        let dump_rows = doc.get("rows").and_then(Value::as_array).unwrap_or(&empty);
        for row in dump_rows {
            rows.push(golden_row(row, keys, pk, ck, multicell, &at)?);
        }
    }
    Ok(rows)
}

fn golden_row(
    row: &Value,
    keys: &[Value],
    pk: &[&str],
    ck: &[&str],
    multicell: &[(&str, Multicell)],
    at: &dyn Fn() -> String,
) -> Result<Row, String> {
    let kind = row.get("type").and_then(Value::as_str).unwrap_or("<none>");
    if kind != "row" {
        return Err(format!(
            "{}: unsupported dump element `{kind}` — a range tombstone or static \
             block is a read-time-reconciliation shape, not an egress-formatting one",
            at()
        ));
    }
    if row.get("deletion_info").is_some() {
        return Err(format!(
            "{}: row deletion marker — the physical dump keeps a row the CLI drops",
            at()
        ));
    }
    let liveness = row.get("liveness_info");
    if let Some(li) = liveness {
        for key in ["ttl", "expires_at", "expired"] {
            if li.get(key).is_some() {
                return Err(format!(
                    "{}: row liveness carries `{key}` — TTL expiry is reconciliation, \
                     not formatting",
                    at()
                ));
            }
        }
    }
    let row_tstamp = liveness
        .and_then(|li| li.get("tstamp"))
        .and_then(Value::as_str);

    let clustering = row
        .get("clustering")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if clustering.len() != ck.len() {
        return Err(format!(
            "{}: golden clustering arity {} but {} clustering column(s) declared ({ck:?})",
            at(),
            clustering.len(),
            ck.len()
        ));
    }

    let mut out: Row = BTreeMap::new();
    for (name, value) in pk.iter().zip(keys.iter()) {
        out.insert((*name).to_string(), value.clone());
    }
    for (name, value) in ck.iter().zip(clustering.iter()) {
        out.insert((*name).to_string(), value.clone());
    }

    let kind_of = |name: &str| -> Option<Multicell> {
        multicell.iter().find(|(n, _)| *n == name).map(|(_, k)| *k)
    };
    let mut multi: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    let empty = Vec::new();
    for cell in row.get("cells").and_then(Value::as_array).unwrap_or(&empty) {
        let name = cell
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{}: cell with no `name`", at()))?;
        for key in ["ttl", "expires_at", "expired"] {
            if cell.get(key).is_some() {
                return Err(format!("{}: cell `{name}` carries `{key}` (TTL)", at()));
            }
        }
        if cell.get("path").is_some() {
            if kind_of(name).is_none() {
                return Err(format!(
                    "{}: cell `{name}` is multi-cell (has a `path`) but no collection \
                     kind is declared for it",
                    at()
                ));
            }
            multi.entry(name.to_string()).or_default().push(cell);
            continue;
        }
        if let Some(del) = cell.get("deletion_info") {
            if cell.get("value").is_some() {
                return Err(format!(
                    "{}: cell `{name}` carries both value and deletion",
                    at()
                ));
            }
            // A complex-column tombstone: Cassandra writes one ahead of a
            // full-collection INSERT (`UnfilteredSerializer` writes the complex
            // deletion before the collection's cells). It shadows only cells
            // older than itself, so it is ignorable ONLY when every cell of this
            // row is strictly newer — asserted, never assumed.
            if kind_of(name).is_none() {
                // A CELL tombstone on a scalar column: the column reconciles to
                // NULL — exactly the "tombstone → null" egress property this lane
                // exists to pin. `sstabledump` keeps the marker; a `SELECT` sees a
                // null. There can be no competing value cell for the same name in
                // the same row (that collision is an error), so no timestamp
                // arbitration is needed.
                if out.insert(name.to_string(), Value::Null).is_some() {
                    return Err(format!(
                        "{}: cell tombstone for `{name}` collides with another cell or \
                         a declared key column",
                        at()
                    ));
                }
                continue;
            }
            let marked = del
                .get("marked_deleted")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("{}: `{name}` deletion with no marked_deleted", at()))?;
            let marked_us = parse_iso_micros(marked)
                .ok_or_else(|| format!("{}: unparseable marked_deleted `{marked}`", at()))?;
            let row_us = row_tstamp
                .and_then(parse_iso_micros)
                .ok_or_else(|| format!("{}: `{name}` deletion but no row liveness tstamp", at()))?;
            if marked_us >= row_us {
                return Err(format!(
                    "{}: complex deletion on `{name}` at {marked} is not older than the \
                     row liveness — it may shadow live cells, so this table is not \
                     comparable against a reconciled result set",
                    at()
                ));
            }
            continue;
        }
        let value = cell
            .get("value")
            .ok_or_else(|| format!("{}: cell `{name}` has no `value`", at()))?;
        if out.insert(name.to_string(), value.clone()).is_some() {
            return Err(format!(
                "{}: cell `{name}` collides with a declared key column",
                at()
            ));
        }
    }

    for (name, cells) in multi {
        let kind = kind_of(&name).ok_or_else(|| format!("{}: `{name}` kind vanished", at()))?;
        let value = match kind {
            // `sstabledump` puts a set element in the cell path.
            Multicell::Set => Value::Array(
                cells
                    .iter()
                    .map(|c| path_head(c, at))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            // A list's path is an internal timeuuid; the element is the value.
            Multicell::List => Value::Array(
                cells
                    .iter()
                    .map(|c| {
                        c.get("value")
                            .cloned()
                            .ok_or_else(|| format!("{}: list cell `{name}` has no value", at()))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            // A map's path is the key, the value is the value.
            Multicell::Map => {
                let mut obj = Map::new();
                for c in &cells {
                    let key = match path_head(c, at)? {
                        Value::String(s) => s,
                        other => other.to_string(),
                    };
                    let value = c
                        .get("value")
                        .cloned()
                        .ok_or_else(|| format!("{}: map cell `{name}` has no value", at()))?;
                    obj.insert(key, value);
                }
                Value::Object(obj)
            }
        };
        if out.insert(name.clone(), value).is_some() {
            return Err(format!(
                "{}: collection `{name}` collides with a declared key column",
                at()
            ));
        }
    }
    Ok(out)
}

fn path_head(cell: &Value, at: &dyn Fn() -> String) -> Result<Value, String> {
    let path = cell
        .get("path")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{}: cell path is not an array", at()))?;
    if path.len() != 1 {
        return Err(format!(
            "{}: cell path arity {} — nested collection paths are out of scope",
            at(),
            path.len()
        ));
    }
    Ok(path[0].clone())
}

/// Microseconds since the Unix epoch for an `sstabledump` ISO-8601 UTC stamp
/// (`YYYY-MM-DDTHH:MM:SS[.frac]Z`), or `None` when unparseable.
///
/// Needed because the complex-deletion guard is an ORDERING question and the
/// goldens mix fraction widths (`.001Z` next to `.378920Z`), which makes a plain
/// string comparison wrong.
pub fn parse_iso_micros(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once(['T', ' '])?;
    let mut d = date.split('-');
    let year: i64 = d.next()?.parse().ok()?;
    let month: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    if d.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, frac),
        None => (time, ""),
    };
    let mut t = hms.split(':');
    let hour: i64 = t.next()?.parse().ok()?;
    let minute: i64 = t.next()?.parse().ok()?;
    let second: i64 = t.next()?.parse().ok()?;
    if t.next().is_some() {
        return None;
    }
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=60).contains(&second) {
        return None;
    }
    if frac.len() > 9 || !frac.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let micros_frac: i64 = if frac.is_empty() {
        0
    } else {
        let padded = format!("{frac:0<6}");
        padded[..6].parse().ok()?
    };
    // days_from_civil (Howard Hinnant's algorithm), exact for the proleptic
    // Gregorian calendar.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days
        .checked_mul(86_400)?
        .checked_add(hour * 3600 + minute * 60 + second)?;
    secs.checked_mul(1_000_000)?.checked_add(micros_frac)
}
